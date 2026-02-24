-- @order_sensitive=true
-- @tags=filter,metrics,self_contained
-- Test Objective:
-- 1. Validate filter metric counting on customer/date predicates.
-- 2. Prevent regressions where this case depends on preloaded SSB data.
-- Test Flow:
-- 1. Create/reset minimal customer and dates tables.
-- 2. Insert deterministic rows covering IN, equality, and year filters.
-- 3. Compute metric counts and order by metric key for stable output.
CREATE DATABASE IF NOT EXISTS sql_tests_d04;
DROP TABLE IF EXISTS sql_tests_d04.t_filter_customer_metrics;
DROP TABLE IF EXISTS sql_tests_d04.t_filter_dates_metrics;
CREATE TABLE sql_tests_d04.t_filter_customer_metrics (
    c_custkey INT,
    c_region VARCHAR(32)
);
CREATE TABLE sql_tests_d04.t_filter_dates_metrics (
    d_datekey INT,
    d_year INT
);

INSERT INTO sql_tests_d04.t_filter_customer_metrics VALUES
    (1, 'ASIA'),
    (2, 'ASIA'),
    (3, 'AMERICA'),
    (4, 'EUROPE'),
    (5, 'AMERICA'),
    (6, 'AFRICA');

INSERT INTO sql_tests_d04.t_filter_dates_metrics VALUES
    (19920101, 1992),
    (19930101, 1993),
    (19931231, 1993),
    (19940101, 1994);

SELECT metric, value
FROM (
    SELECT 'asia_america_customers' AS metric, COUNT(*) AS value
    FROM sql_tests_d04.t_filter_customer_metrics
    WHERE c_region IN ('ASIA', 'AMERICA')
    UNION ALL
    SELECT 'asia_customers', COUNT(*)
    FROM sql_tests_d04.t_filter_customer_metrics
    WHERE c_region = 'ASIA'
    UNION ALL
    SELECT 'datekey_19920101', COUNT(*)
    FROM sql_tests_d04.t_filter_dates_metrics
    WHERE d_datekey = 19920101
    UNION ALL
    SELECT 'dates_1993', COUNT(*)
    FROM sql_tests_d04.t_filter_dates_metrics
    WHERE d_year = 1993
) t
ORDER BY metric;
