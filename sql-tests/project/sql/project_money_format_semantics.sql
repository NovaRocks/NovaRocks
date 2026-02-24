-- @order_sensitive=true
-- @tags=project,string
-- Test Objective:
-- 1. Validate MONEY_FORMAT formatting for decimal, bigint, and double inputs.
-- 2. Prevent regressions in thousands separators and double-style fractional formatting.
-- Test Flow:
-- 1. Evaluate MONEY_FORMAT on numeric constants with explicit types.
-- 2. Compare decimal/bigint outputs and double outputs in the same row.
-- 3. Assert deterministic scalar outputs.
SELECT
  MONEY_FORMAT(12345.678) AS mf_decimal_literal,
  MONEY_FORMAT(CAST(12345 AS BIGINT)) AS mf_bigint,
  MONEY_FORMAT(CAST(0.015 AS DOUBLE)) AS mf_double_small,
  MONEY_FORMAT(CAST(-0.004 AS DOUBLE)) AS mf_double_negative_zero;
