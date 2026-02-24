-- @order_sensitive=true
-- @tags=d04,project,date,date_add,months_add,null_semantics
-- Test Objective:
-- 1. Validate invalid datetime and non-numeric interval inputs return NULL for date add variants.
-- 2. Prevent regressions where date_add family throws type errors instead of producing NULL.
-- Test Flow:
-- 1. Execute days_add/date_add/months_add with invalid datetime and interval literals.
-- 2. Assert all outputs are NULL via IS NULL checks.
SELECT
  DAYS_ADD('abcd', 1) IS NULL AS days_add_invalid_date_is_null_v,
  DAYS_ADD('2024-01-01', 'x') IS NULL AS days_add_invalid_delta_is_null_v,
  DATE_ADD('abcd', 1) IS NULL AS date_add_invalid_date_is_null_v,
  MONTHS_ADD('abcd', 1) IS NULL AS months_add_invalid_date_is_null_v,
  MONTHS_ADD('2024-01-01', 'x') IS NULL AS months_add_invalid_delta_is_null_v;
