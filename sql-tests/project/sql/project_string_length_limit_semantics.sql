-- @order_sensitive=true
-- @tags=project,string,length_limit
-- Test Objective:
-- 1. Validate OLAP string max-length boundary behavior for REPEAT/SPACE/LPAD/RPAD/CONCAT/CONCAT_WS.
-- 2. Prevent regressions where over-limit string builders return oversized values instead of NULL.
-- Test Flow:
-- 1. Evaluate boundary-sized outputs at 1048576 bytes.
-- 2. Evaluate over-limit outputs at 1048577 bytes.
-- 3. Assert boundary lengths and NULL markers in a deterministic single-row projection.
SELECT
  LENGTH(REPEAT('a', 1048576)) AS repeat_limit_len,
  REPEAT('a', 1048577) IS NULL AS repeat_over_limit_is_null,
  LENGTH(SPACE(1048576)) AS space_limit_len,
  SPACE(1048577) IS NULL AS space_over_limit_is_null,
  LENGTH(LPAD('x', 1048576, 'ab')) AS lpad_limit_len,
  LPAD('x', 1048577, 'ab') IS NULL AS lpad_over_limit_is_null,
  LENGTH(RPAD('x', 1048576, 'ab')) AS rpad_limit_len,
  RPAD('x', 1048577, 'ab') IS NULL AS rpad_over_limit_is_null,
  LENGTH(CONCAT(REPEAT('a', 600000), REPEAT('b', 448576))) AS concat_limit_len,
  CONCAT(REPEAT('a', 600000), REPEAT('b', 448577)) IS NULL AS concat_over_limit_is_null,
  LENGTH(CONCAT_WS('-', REPEAT('a', 600000), REPEAT('b', 448575))) AS concat_ws_limit_len,
  CONCAT_WS('-', REPEAT('a', 600000), REPEAT('b', 448576)) IS NULL AS concat_ws_over_limit_is_null;
