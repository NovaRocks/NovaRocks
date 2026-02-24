-- @order_sensitive=true
-- @tags=complex,array
-- Test Objective:
-- 1. Validate core ARRAY scalar functions.
-- 2. Prevent regressions in ARRAY length/sum/contains/position semantics.
-- Test Flow:
-- 1. Build constant ARRAY expressions.
-- 2. Evaluate core ARRAY functions.
-- 3. Assert scalar output.
SELECT
    ARRAY_LENGTH([1, 2, 3]) AS len_a,
    ARRAY_SUM([1, 2, 3]) AS sum_a,
    ARRAY_CONTAINS([1, 2, 3], 2) AS has_2,
    ARRAY_POSITION([1, 2, 3], 3) AS pos_3;
