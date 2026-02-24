-- @order_sensitive=true
-- @tags=complex,array
-- Test Objective:
-- 1. Validate ARRAY_SORT and ARRAY_JOIN outputs.
-- 2. Prevent regressions in array ordering/stringification helpers.
-- Test Flow:
-- 1. Build deterministic array literals.
-- 2. Apply sort and join helpers.
-- 3. Assert scalar output.
SELECT
    ARRAY_SORT([3, 1, 2, NULL]) AS sorted_arr,
    ARRAY_JOIN(['a', 'b', NULL], ',') AS joined_arr,
    ARRAY_CUM_SUM([1, 2, 3]) AS cumsum_arr;
