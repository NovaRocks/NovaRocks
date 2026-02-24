-- @order_sensitive=true
-- @tags=join,float,null_semantics
-- Test Objective:
-- 1. Validate non-finite floating-point expressions are normalized to NULL.
-- 2. Prevent regressions where NaN/Infinity keys are matched by equality hash join.
-- Test Flow:
-- 1. Build deterministic left/right inline datasets containing non-finite and finite keys.
-- 2. Assert non-finite keys become NULL on both sides.
-- 3. Assert INNER JOIN on equality only matches the finite key.
WITH
left_keys AS (
  SELECT CAST('NaN' AS DOUBLE) AS k
  UNION ALL SELECT CAST('Infinity' AS DOUBLE)
  UNION ALL SELECT SQRT(-1.0)
  UNION ALL SELECT 1.5
),
right_keys AS (
  SELECT CAST('NaN' AS DOUBLE) AS k
  UNION ALL SELECT LOG(-1.0)
  UNION ALL SELECT ACOS(2.0)
  UNION ALL SELECT 1.5
)
SELECT
  (SELECT COUNT(*) FROM left_keys WHERE k IS NULL) AS left_null_keys,
  (SELECT COUNT(*) FROM right_keys WHERE k IS NULL) AS right_null_keys,
  (SELECT COUNT(*) FROM left_keys l INNER JOIN right_keys r ON l.k = r.k) AS join_matches;
