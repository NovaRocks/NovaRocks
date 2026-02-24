-- @order_sensitive=true
-- @tags=complex,array
-- Test Objective:
-- 1. Validate ARRAY_SLICE/ARRAY_CONCAT/ARRAY_DISTINCT behavior.
-- 2. Prevent regressions in array-shape transformation functions.
-- Test Flow:
-- 1. Build deterministic array expressions.
-- 2. Apply slice/concat/distinct.
-- 3. Assert scalar output arrays.
SELECT
    ARRAY_SLICE([1, 2, 3, 4], 2, 2) AS sliced,
    ARRAY_CONCAT([1, 2], [3, NULL]) AS concated,
    ARRAY_DISTINCT([1, 2, 2, NULL, 1, NULL]) AS deduped;
