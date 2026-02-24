-- @order_sensitive=true
-- @tags=project,date
-- Test Objective:
-- 1. Validate TO_DAYS/FROM_DAYS round-trip semantics.
-- 2. Validate FROM_DAYS accepts integer day values from BIGINT expressions.
-- Test Flow:
-- 1. Compute TO_DAYS for a fixed date.
-- 2. Apply FROM_DAYS to the computed result and BIGINT input.
-- 3. Assert deterministic scalar outputs.
SELECT
  TO_DAYS(DATE '2024-01-10') AS days_v,
  FROM_DAYS(TO_DAYS(DATE '2024-01-10')) AS roundtrip_v,
  FROM_DAYS(CAST(739260 AS BIGINT)) AS bigint_input_v;
