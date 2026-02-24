-- @order_sensitive=true
-- @tags=project,cast,time
-- Test Objective:
-- 1. Validate CAST(... AS TIME) literal parsing semantics.
-- 2. Distinguish direct string-to-time cast from datetime-to-time cast path.
-- Test Flow:
-- 1. Cast canonical and extended-hour time strings.
-- 2. Cast datetime/date values to TIME.
-- 3. Assert deterministic scalar output.
SELECT
  CAST('00:00:00' AS TIME) AS t0,
  CAST('25:00:00' AS TIME) AS t25,
  CAST('1970-01-01 01:01:01' AS TIME) AS t_dt_literal,
  CAST(CAST('1970-01-01 01:01:01' AS DATETIME) AS TIME) AS t_from_datetime,
  CAST(CAST('2020-01-02' AS DATE) AS TIME) AS t_from_date;
