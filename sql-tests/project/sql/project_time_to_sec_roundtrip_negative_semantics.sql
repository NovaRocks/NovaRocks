-- @order_sensitive=true
-- @tags=project,date,time,roundtrip
-- Test Objective:
-- 1. Validate TIME_TO_SEC(SEC_TO_TIME(x)) semantics for negative values.
-- 2. Ensure sec_to_time saturation is preserved by time_to_sec roundtrip.
-- 3. Keep negative time literals strict (still NULL for direct string literal).
-- Test Flow:
-- 1. Evaluate sec_to_time on negative and overflow-negative inputs.
-- 2. Apply time_to_sec on sec_to_time outputs and compare with expected seconds.
-- 3. Validate direct negative literal remains NULL.
SELECT
  SEC_TO_TIME(-1) AS s_neg1,
  TIME_TO_SEC(SEC_TO_TIME(-1)) AS rt_neg1,
  SEC_TO_TIME(-2147483648) AS s_floor_cap,
  TIME_TO_SEC(SEC_TO_TIME(-2147483648)) AS rt_floor_cap,
  TIME_TO_SEC('-00:00:01') AS literal_neg;
