-- @order_sensitive=true
-- @tags=project,date
-- Test Objective:
-- 1. Validate date/time scalar semantics for coercion, boundary handling, and null propagation.
-- 2. Guard regressions for hour_from_unixtime, makedate, microseconds_add/sub, time_format, and years_diff.
-- Test Flow:
-- 1. Execute one deterministic projection over constant literals.
-- 2. Assert exact scalar outputs and nullability.
SELECT
  HOUR_FROM_UNIXTIME(1700000000) AS hour_unix_v,
  HOUR_FROM_UNIXTIME(-1) AS hour_unix_neg_v,
  MAKEDATE('2024', 60.9) AS makedate_coerce_v,
  MAKEDATE(2024, -1) AS makedate_neg_v,
  MICROSECONDS_ADD('2024-02-29 12:00:00', '123456') AS micros_add_v,
  MICROSECONDS_SUB('2024-02-29 12:00:00', 123456.9) AS micros_sub_v,
  TIME_FORMAT('12:34:56', '%H:%i:%s') AS time_fmt_time_v,
  TIME_FORMAT('2024-02-29 12:34:56', '%H:%i:%s') AS time_fmt_datetime_v,
  YEARS_DIFF('2026-02-28', '2024-02-29') AS years_diff_v,
  YEARS_DIFF('2024-02-29', '2026-02-28') AS years_diff_neg_v;
