-- @order_sensitive=true
-- @tags=aggregate,row_count,self_contained
-- Test Objective:
-- 1. Validate row-count aggregation across multiple base tables.
-- 2. Prevent regressions where this case depends on SSB schema presence.
-- Test Flow:
-- 1. Create/reset five minimal source tables.
-- 2. Insert deterministic row counts per table.
-- 3. Union all COUNT(*) metrics and compare ordered output.
CREATE DATABASE IF NOT EXISTS sql_tests_d06;
DROP TABLE IF EXISTS sql_tests_d06.t_agg_count_customer;
DROP TABLE IF EXISTS sql_tests_d06.t_agg_count_dates;
DROP TABLE IF EXISTS sql_tests_d06.t_agg_count_lineorder;
DROP TABLE IF EXISTS sql_tests_d06.t_agg_count_part;
DROP TABLE IF EXISTS sql_tests_d06.t_agg_count_supplier;
CREATE TABLE sql_tests_d06.t_agg_count_customer (id INT);
CREATE TABLE sql_tests_d06.t_agg_count_dates (id INT);
CREATE TABLE sql_tests_d06.t_agg_count_lineorder (id INT);
CREATE TABLE sql_tests_d06.t_agg_count_part (id INT);
CREATE TABLE sql_tests_d06.t_agg_count_supplier (id INT);

INSERT INTO sql_tests_d06.t_agg_count_customer VALUES
    (1),
    (2),
    (3),
    (4);
INSERT INTO sql_tests_d06.t_agg_count_dates VALUES
    (1),
    (2),
    (3);
INSERT INTO sql_tests_d06.t_agg_count_lineorder VALUES
    (1),
    (2),
    (3),
    (4),
    (5);
INSERT INTO sql_tests_d06.t_agg_count_part VALUES
    (1),
    (2);
INSERT INTO sql_tests_d06.t_agg_count_supplier VALUES
    (1),
    (2),
    (3);

SELECT table_name, row_count
FROM (
    SELECT 'customer' AS table_name, COUNT(*) AS row_count FROM sql_tests_d06.t_agg_count_customer
    UNION ALL
    SELECT 'dates', COUNT(*) FROM sql_tests_d06.t_agg_count_dates
    UNION ALL
    SELECT 'lineorder', COUNT(*) FROM sql_tests_d06.t_agg_count_lineorder
    UNION ALL
    SELECT 'part', COUNT(*) FROM sql_tests_d06.t_agg_count_part
    UNION ALL
    SELECT 'supplier', COUNT(*) FROM sql_tests_d06.t_agg_count_supplier
) t
ORDER BY table_name;
