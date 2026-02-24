-- @order_sensitive=true
-- @tags=sort,float,null_semantics
-- Test Objective:
-- 1. Validate ORDER BY behavior when float expressions can produce non-finite values.
-- 2. Prevent regressions where NaN/Infinity leak into result/order instead of SQL NULL semantics.
-- Test Flow:
-- 1. Build a small in-memory set with CAST and math expressions that can produce non-finite values.
-- 2. Sort with explicit NULL ordering and deterministic tie-breakers.
-- 3. Assert that non-finite branches are treated as NULL in output ordering.
WITH t AS (
  SELECT 1 AS id, CAST('NaN' AS DOUBLE) AS d
  UNION ALL SELECT 2, CAST('Infinity' AS DOUBLE)
  UNION ALL SELECT 3, CAST('-Infinity' AS DOUBLE)
  UNION ALL SELECT 4, SQRT(-1.0)
  UNION ALL SELECT 5, LOG(-1.0)
  UNION ALL SELECT 6, ACOS(2.0)
  UNION ALL SELECT 7, 1.25
  UNION ALL SELECT 8, -2.5
)
SELECT id, d
FROM t
ORDER BY d ASC NULLS FIRST, id ASC;
