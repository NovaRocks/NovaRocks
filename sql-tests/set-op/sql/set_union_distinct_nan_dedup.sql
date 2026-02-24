-- @order_sensitive=true
-- @tags=set_op,union_distinct,float,nan
-- Test Objective:
-- 1. Validate UNION DISTINCT deduplicates FLOAT/DOUBLE NaN values.
-- 2. Prevent hash-key equality regressions where NaN rows are treated as distinct.
-- Test Flow:
-- 1. Build two identical NaN rows via constant SELECTs.
-- 2. Apply UNION DISTINCT.
-- 3. Assert only one distinct row remains.
SELECT COUNT(*) AS distinct_nan_count
FROM (
    SELECT CAST('NaN' AS DOUBLE) AS v
    UNION
    SELECT CAST('NaN' AS DOUBLE) AS v
) t;
