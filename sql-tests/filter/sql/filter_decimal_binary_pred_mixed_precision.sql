-- @order_sensitive=true
-- @tags=filter,comparison,decimal,child_type
-- Test Objective:
-- 1. Validate decimal BINARY_PRED with mixed precision/scale operands.
-- 2. Prevent regressions where FE child_type=DECIMAL64 rejects compatible decimal children.
-- Test Flow:
-- 1. Build two decimal rows with scale 2.
-- 2. Compare against decimal literals with different precision/scale using >=, <= and BETWEEN.
-- 3. Assert deterministic boolean outputs in ascending decimal order.
WITH t(v) AS (
  SELECT CAST(120.00 AS DECIMAL(7,2))
  UNION ALL
  SELECT CAST(90.00 AS DECIMAL(7,2))
)
SELECT
  v,
  v >= CAST(100 AS DECIMAL(5,0)) AS ge_lower,
  v <= CAST(150.000 AS DECIMAL(9,3)) AS le_upper,
  v BETWEEN CAST(100.0 AS DECIMAL(6,1)) AND CAST(150 AS DECIMAL(4,0)) AS between_flag
FROM t
ORDER BY v;
