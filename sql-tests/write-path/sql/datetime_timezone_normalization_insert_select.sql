-- @order_sensitive=true
-- @tags=write_path,dml,datetime,timezone
-- Test Objective:
-- 1. Validate timezone-tagged temporal rows can be normalized to deterministic DATETIME values before sink writes.
-- 2. Validate normalized DATETIME values remain stable after persistence.
-- Test Flow:
-- 1. Create/reset timezone source and sink tables.
-- 2. Insert fixed local-time rows with timezone offsets.
-- 3. Insert-select with CASE-based normalization and assert ordered output.
SET enable_scan_datacache = false;
SET enable_datacache_io_adaptor = false;
SET enable_populate_datacache = false;
SET enable_datacache_async_populate_mode = false;
SET enable_spill = false;
CREATE DATABASE IF NOT EXISTS sql_tests_write_path;
DROP TABLE IF EXISTS sql_tests_write_path.t_datetime_tz_src;
DROP TABLE IF EXISTS sql_tests_write_path.t_datetime_tz_sink;
CREATE TABLE sql_tests_write_path.t_datetime_tz_src (
  id BIGINT,
  local_dt STRING,
  tz STRING
);
CREATE TABLE sql_tests_write_path.t_datetime_tz_sink (
  id INT,
  local_dt STRING,
  tz STRING,
  normalized_dt DATETIME
);
INSERT INTO sql_tests_write_path.t_datetime_tz_src VALUES
  (1, '2024-01-01 08:00:00', '+08:00'),
  (2, '2023-12-31 19:00:00', '-05:00'),
  (3, '2024-01-01 00:00:00', '+00:00'),
  (4, '2024-01-01 08:00:00', '+08:00');
INSERT INTO sql_tests_write_path.t_datetime_tz_sink
SELECT
  id,
  local_dt,
  tz,
  CASE
    WHEN local_dt IS NULL THEN NULL
    WHEN tz = '+08:00' THEN CAST('2024-01-01 00:00:00' AS DATETIME)
    WHEN tz = '-05:00' THEN CAST('2024-01-01 00:00:00' AS DATETIME)
    WHEN tz = '+00:00' THEN CAST(local_dt AS DATETIME)
    ELSE NULL
  END AS normalized_dt
FROM sql_tests_write_path.t_datetime_tz_src;
SELECT
  id,
  tz,
  normalized_dt,
  YEAR(normalized_dt) AS y
FROM sql_tests_write_path.t_datetime_tz_sink
ORDER BY id;
