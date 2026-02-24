-- @order_sensitive=true
-- @tags=set_op,union,decimal,const,scale
-- Test Objective:
-- 1. Validate UNION DISTINCT handles mixed decimal literal scales in const rows.
-- 2. Prevent regressions where const row decimals are materialized with mismatched precision.
-- Test Flow:
-- 1. Build two constant decimal expressions with different inferred precision.
-- 2. Apply UNION (DISTINCT) to force set-op grouping on the unified output slot.
-- 3. Assert that only one distinct row remains.
SELECT COUNT(*) AS dedup_count
FROM (
    SELECT (-1.0) * 0.0 AS v
    UNION
    SELECT 0.0 AS v
) t;
