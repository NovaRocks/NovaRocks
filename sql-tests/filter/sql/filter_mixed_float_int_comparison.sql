-- @order_sensitive=true
-- @tags=filter,comparison,numeric
-- Test Objective:
-- 1. Validate mixed float/int comparison for scalar predicates.
-- 2. Prevent regressions where numeric comparison rejects Float64 vs Int32.
-- Test Flow:
-- 1. Build scalar expressions with ABS over integer arithmetic.
-- 2. Compare the result with integer literals.
-- 3. Assert deterministic boolean outputs.
SELECT
  abs(1 - 2) = 0 AS eq_zero,
  abs(1 - 2) = 1 AS eq_one;
