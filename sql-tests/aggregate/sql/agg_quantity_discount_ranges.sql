-- @order_sensitive=true
-- @tags=aggregate,min_max,self_contained
-- Test Objective:
-- 1. Validate MIN/MAX aggregation on lineorder-like numeric columns.
-- 2. Prevent regressions where this case relies on external SSB base tables.
-- Test Flow:
-- 1. Create/reset a minimal lineorder-like table.
-- 2. Insert deterministic quantity/discount rows with explicit boundaries.
-- 3. Compute MIN/MAX and compare a single deterministic row.
CREATE DATABASE IF NOT EXISTS sql_tests_d06;
DROP TABLE IF EXISTS sql_tests_d06.t_agg_quantity_discount_ranges;
CREATE TABLE sql_tests_d06.t_agg_quantity_discount_ranges (
    lo_quantity INT,
    lo_discount INT
);

INSERT INTO sql_tests_d06.t_agg_quantity_discount_ranges VALUES
    (1, 0),
    (50, 10),
    (20, 3),
    (7, 5),
    (42, 2);

SELECT
    MIN(lo_quantity) AS min_qty,
    MAX(lo_quantity) AS max_qty,
    MIN(lo_discount) AS min_discount,
    MAX(lo_discount) AS max_discount
FROM sql_tests_d06.t_agg_quantity_discount_ranges;
