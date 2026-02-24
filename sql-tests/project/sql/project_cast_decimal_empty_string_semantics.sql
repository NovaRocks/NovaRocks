-- @order_sensitive=true
-- @tags=project,cast,decimal
-- Test Objective:
-- 1. Validate CAST from empty/blank VARCHAR to DECIMAL returns NULL.
-- 2. Prevent regressions where empty strings are coerced to zero.
-- Test Flow:
-- 1. Cast empty string, blank string, numeric string, and NULL to DECIMAL(10,2).
-- 2. Assert only valid numeric input yields non-NULL decimal value.
SELECT
  CAST('' AS DECIMAL(10,2)) AS empty_dec,
  CAST(' ' AS DECIMAL(10,2)) AS blank_dec,
  CAST('0' AS DECIMAL(10,2)) AS zero_dec,
  CAST(NULL AS DECIMAL(10,2)) AS null_dec;
