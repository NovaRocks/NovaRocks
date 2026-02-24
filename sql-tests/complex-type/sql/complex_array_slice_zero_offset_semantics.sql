-- @order_sensitive=true
-- @tags=complex,array
-- Test Objective:
-- 1. Validate ARRAY_SLICE offset=0 semantics match StarRocks (empty result).
-- 2. Prevent regressions where offset=0 is treated as start-from-first-element.
-- Test Flow:
-- 1. Build deterministic array literals.
-- 2. Evaluate ARRAY_SLICE with zero, positive, and negative offsets.
-- 3. Assert projected arrays.
SELECT
    ARRAY_SLICE([1, 2, 3], 0, 2) AS sliced_zero_with_len,
    ARRAY_SLICE([1, 2, 3], 0) AS sliced_zero_no_len,
    ARRAY_SLICE([1, 2, 3], -2, 2) AS sliced_negative,
    ARRAY_SLICE([1, 2, 3], 1, 2) AS sliced_positive;
