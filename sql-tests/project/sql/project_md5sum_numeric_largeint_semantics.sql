-- @order_sensitive=true
-- @tags=project,encryption,largeint
-- Test Objective:
-- 1. Validate md5sum_numeric returns LARGEINT-compatible 128-bit signed values.
-- 2. Prevent regressions that truncate md5sum_numeric output to BIGINT width.
-- Test Flow:
-- 1. Evaluate md5sum_numeric on stable literal inputs.
-- 2. Cover both negative and positive 128-bit outputs.
-- 3. Validate NULL propagation.
SELECT
  md5sum_numeric('abc') AS n1,
  md5sum_numeric('ab') AS n2,
  md5sum_numeric(CAST(NULL AS STRING)) AS nnull;
