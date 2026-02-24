-- @order_sensitive=true
-- @tags=project,cast,boolean,varchar
-- Test Objective:
-- 1. Validate StarRocks-compatible semantics for CAST(VARCHAR AS BOOLEAN) on numeric strings.
-- 2. Ensure IF condition behavior matches FE/BE semantics when condition is VARCHAR.
-- Test Flow:
-- 1. Cast signed/space-prefixed numeric strings to BOOLEAN.
-- 2. Assert invalid and overflow VARCHAR values become NULL.
-- 3. Verify IF(condition, then, else) follows the same boolean conversion results.
SELECT
  CAST('2' AS BOOLEAN) AS cast_two,
  CAST('-1' AS BOOLEAN) AS cast_minus_one,
  CAST('+0' AS BOOLEAN) AS cast_plus_zero,
  CAST('false' AS BOOLEAN) AS cast_false,
  CAST('TRUE' AS BOOLEAN) AS cast_true_upper,
  CAST('t' AS BOOLEAN) IS NULL AS cast_t_is_null,
  CAST('2147483648' AS BOOLEAN) IS NULL AS cast_i32_overflow_is_null,
  CAST(' 2' AS BOOLEAN) AS cast_space_two,
  IF('2', 1, 0) AS if_two,
  IF('-1', 1, 0) AS if_minus_one,
  IF('+0', 1, 0) AS if_plus_zero,
  IF('t', 1, 0) AS if_t,
  IF(' 2', 1, 0) AS if_space_two;
