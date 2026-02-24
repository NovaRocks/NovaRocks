-- @order_sensitive=true
-- @tags=project,function,abs,largeint
-- Test Objective:
-- 1. Validate ABS on LARGEINT boundary values follows FE planned LARGEINT semantics.
-- 2. Prevent BIGINT-overflow style regression where ABS(min_bigint_literal) wraps to negative.
-- Test Flow:
-- 1. Execute deterministic projection-only query (no table dependency).
-- 2. Assert ABS result, comparison against BIGINT max, and +1 arithmetic on the same row.
SELECT
  ABS(-9223372036854775808) AS abs_largeint_min,
  ABS(-9223372036854775808) > 9223372036854775807 AS gt_bigint_max,
  ABS(-9223372036854775808) + 1 AS abs_largeint_min_plus_one;
