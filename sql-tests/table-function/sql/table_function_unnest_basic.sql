-- @order_sensitive=true
-- @tags=table_function,unnest
-- Test Objective:
-- 1. Validate basic CROSS JOIN LATERAL UNNEST row expansion.
-- 2. Prevent regressions in table-function output cardinality.
-- Test Flow:
-- 1. Build single-row array source.
-- 2. Expand with UNNEST.
-- 3. Assert ordered output rows.
SELECT x
FROM (
    SELECT [1, 2, 3] AS arr
) t
CROSS JOIN LATERAL UNNEST(t.arr) AS u(x)
ORDER BY x;
