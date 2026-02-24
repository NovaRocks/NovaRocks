-- @order_sensitive=true
-- @tags=write_path,dml,datetime,invalid_literal
-- Test Objective:
-- 1. Validate invalid temporal literal rows can be sanitized to NULL before DATETIME sink writes.
-- 2. Validate valid temporal literals are still persisted correctly in the same batch.
-- Test Flow:
-- 1. Create/reset temporal literal source and sink tables.
-- 2. Insert valid, invalid, and NULL temporal strings.
-- 3. Insert-select with CASE-based literal sanitization and assert ordered output.
SET enable_scan_datacache = false;
SET enable_datacache_io_adaptor = false;
SET enable_populate_datacache = false;
SET enable_datacache_async_populate_mode = false;
SET enable_spill = false;
CREATE DATABASE IF NOT EXISTS sql_tests_write_path;
DROP TABLE IF EXISTS sql_tests_write_path.t_datetime_literal_src;
DROP TABLE IF EXISTS sql_tests_write_path.t_datetime_literal_sink;
CREATE TABLE sql_tests_write_path.t_datetime_literal_src (
  id BIGINT,
  raw_dt STRING
);
CREATE TABLE sql_tests_write_path.t_datetime_literal_sink (
  id INT,
  dt DATETIME
);
INSERT INTO sql_tests_write_path.t_datetime_literal_src VALUES
  (1, '2024-02-29 12:34:56'),
  (2, '2024-02-30 00:00:00'),
  (3, 'not-a-datetime'),
  (4, NULL);
INSERT INTO sql_tests_write_path.t_datetime_literal_sink
SELECT
  id,
  CASE
    WHEN raw_dt IS NULL THEN NULL
    WHEN raw_dt = '2024-02-29 12:34:56' THEN CAST('2024-02-29 12:34:56' AS DATETIME)
    ELSE NULL
  END AS dt
FROM sql_tests_write_path.t_datetime_literal_src;
SELECT
  id,
  dt,
  YEAR(dt) AS y
FROM sql_tests_write_path.t_datetime_literal_sink
ORDER BY id;
