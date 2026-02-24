-- @order_sensitive=true
-- @tags=d04,project,date,date_trunc,months_diff
-- Test Objective:
-- 1. Validate StarRocks-compatible boundary semantics for months_diff.
-- 2. Validate date_trunc millisecond/microsecond precision behavior.
-- Test Flow:
-- 1. Execute months_diff on day/time edge boundaries.
-- 2. Execute date_trunc with millisecond and microsecond units.
-- 3. Assert one-row deterministic projection output.
SELECT
  MONTHS_DIFF('2024-01-31 00:00:00', '2024-02-01 00:00:00') AS months_diff_neg_boundary_v,
  MONTHS_DIFF('2024-02-01 00:00:00', '2024-01-31 00:00:00') AS months_diff_pos_boundary_v,
  MONTHS_DIFF('2024-02-29 12:00:00', '2024-01-29 12:00:01') AS months_diff_time_boundary_v,
  DATE_TRUNC('millisecond', '2024-01-03 03:04:05.123456') AS date_trunc_millisecond_v,
  DATE_TRUNC('microsecond', '2024-01-03 03:04:05.123456') AS date_trunc_microsecond_v;
