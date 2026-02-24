-- @order_sensitive=true
-- @tags=sort,float,null_semantics,case
-- Test Objective:
-- 1. Validate ORDER BY behavior when CASE branches include math expressions that can produce non-finite float values.
-- 2. Ensure non-finite branch results are normalized to NULL before sorting.
-- Test Flow:
-- 1. Build deterministic rows with nullable float values.
-- 2. Compute CASE output with SQRT/LOG/LN/ACOS/EXP branches.
-- 3. Sort with explicit NULL ordering and deterministic tie-breakers.
WITH t AS (
  SELECT 1 AS id, CAST(NULL AS DOUBLE) AS d
  UNION ALL SELECT 2, -3.5
  UNION ALL SELECT 3, 0.0
  UNION ALL SELECT 4, 7.25
  UNION ALL SELECT 5, -7.25
  UNION ALL SELECT 6, 1.0
  UNION ALL SELECT 7, 3.5
  UNION ALL SELECT 8, NULL
  UNION ALL SELECT 9, 2.2
  UNION ALL SELECT 10, -0.0
)
SELECT id,
       CASE
         WHEN id = 1 THEN SQRT(-1.0)
         WHEN id = 2 THEN LOG(-1.0)
         WHEN id = 3 THEN LN(0.0)
         WHEN id = 4 THEN ACOS(2.0)
         WHEN id = 5 THEN EXP(1000)
         ELSE d
       END AS k
FROM t
ORDER BY k DESC NULLS LAST, id ASC;
