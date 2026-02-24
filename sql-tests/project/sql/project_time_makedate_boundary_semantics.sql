-- @order_sensitive=true
-- @tags=project,date,time,makedate
-- Test Objective:
-- 1. Validate SEC_TO_TIME/TIME_TO_SEC boundary semantics.
-- 2. Validate MAKEDATE overflow and year-zero behavior.
-- Test Flow:
-- 1. Evaluate time conversion functions on negative/overflow inputs.
-- 2. Evaluate MAKEDATE on invalid day-of-year and year zero.
-- 3. Assert deterministic scalar output.
SELECT
  SEC_TO_TIME(-1) AS s_neg1,
  SEC_TO_TIME(90061) AS s_90061,
  SEC_TO_TIME(3020399) AS s_cap_838,
  SEC_TO_TIME(3020400) AS s_839,
  SEC_TO_TIME(3023999) AS s_cap_839,
  TIME_TO_SEC('00:00:00') AS t0,
  TIME_TO_SEC('23:59:59') AS t_max,
  TIME_TO_SEC('-00:00:01') AS t_neg1,
  TIME_TO_SEC('25:00:00') AS t_25h,
  TIME_TO_SEC('0000-00-00 23:59:59') AS t_date_prefix,
  TIME_TO_SEC('1970-01-01 01:01:01') AS t_datetime_literal,
  MAKEDATE(2020, 367) AS md_over,
  MAKEDATE(2020, 0) AS md_zero,
  MAKEDATE(0, 1) AS md_y0,
  MAKEDATE(10000, 1) AS md_y_over,
  MAKEDATE(-1, 1) AS md_y_neg;
