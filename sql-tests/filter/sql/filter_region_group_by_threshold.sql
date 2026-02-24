-- @order_sensitive=true
-- @tags=filter,group_by,self_contained
-- Test Objective:
-- 1. Validate filtered GROUP BY counting with a numeric threshold predicate.
-- 2. Prevent regressions where this case assumes SSB customer table availability.
-- Test Flow:
-- 1. Create/reset a minimal customer-like table.
-- 2. Insert deterministic key/region rows around the threshold.
-- 3. Aggregate filtered rows by region and order result deterministically.
CREATE DATABASE IF NOT EXISTS sql_tests_d04;
DROP TABLE IF EXISTS sql_tests_d04.t_filter_group_by_customer;
CREATE TABLE sql_tests_d04.t_filter_group_by_customer (
    c_custkey INT,
    c_region VARCHAR(32)
);

INSERT INTO sql_tests_d04.t_filter_group_by_customer VALUES
    (10000, 'AFRICA'),
    (15001, 'AFRICA'),
    (15002, 'AMERICA'),
    (15003, 'AMERICA'),
    (14999, 'ASIA'),
    (17000, 'ASIA'),
    (18000, 'EUROPE'),
    (13000, 'MIDDLE EAST'),
    (19000, 'MIDDLE EAST');

SELECT c_region, COUNT(*) AS customer_count
FROM sql_tests_d04.t_filter_group_by_customer
WHERE c_custkey > 15000
GROUP BY c_region
ORDER BY c_region;
