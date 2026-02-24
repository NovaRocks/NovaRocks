-- @order_sensitive=true
-- @tags=write_path,dml,decimal,overflow
-- Test Objective:
-- 1. Validate overflow-range decimal rows can be sanitized to NULL before sink writes.
-- 2. Validate in-range boundary rows are still written after scale narrowing.
-- Test Flow:
-- 1. Create/reset decimal sink table.
-- 2. Insert boundary and overflow decimal rows via typed SELECT constants.
-- 3. Apply CASE-based overflow guard and assert ordered sink output.
SET enable_scan_datacache = false;
SET enable_datacache_io_adaptor = false;
SET enable_populate_datacache = false;
SET enable_datacache_async_populate_mode = false;
SET enable_spill = false;
CREATE DATABASE IF NOT EXISTS sql_tests_write_path;
DROP TABLE IF EXISTS sql_tests_write_path.t_decimal_overflow_sink;
CREATE TABLE sql_tests_write_path.t_decimal_overflow_sink (
  id INT,
  v DECIMAL(10, 2)
);
INSERT INTO sql_tests_write_path.t_decimal_overflow_sink
SELECT
  CAST(1 AS INT),
  CASE
    WHEN ABS(CAST(99999999.9949 AS DECIMAL(13, 4))) > 99999999.9999 THEN NULL
    ELSE CAST(99999999.9949 AS DECIMAL(13, 4))
  END;
INSERT INTO sql_tests_write_path.t_decimal_overflow_sink
SELECT
  CAST(2 AS INT),
  CASE
    WHEN ABS(CAST(-99999999.9949 AS DECIMAL(13, 4))) > 99999999.9999 THEN NULL
    ELSE CAST(-99999999.9949 AS DECIMAL(13, 4))
  END;
INSERT INTO sql_tests_write_path.t_decimal_overflow_sink
SELECT
  CAST(3 AS INT),
  CASE
    WHEN ABS(CAST(100000000.0000 AS DECIMAL(13, 4))) > 99999999.9999 THEN NULL
    ELSE CAST(100000000.0000 AS DECIMAL(13, 4))
  END;
INSERT INTO sql_tests_write_path.t_decimal_overflow_sink
SELECT
  CAST(4 AS INT),
  CASE
    WHEN ABS(CAST(-100000000.0000 AS DECIMAL(13, 4))) > 99999999.9999 THEN NULL
    ELSE CAST(-100000000.0000 AS DECIMAL(13, 4))
  END;
INSERT INTO sql_tests_write_path.t_decimal_overflow_sink
SELECT
  CAST(5 AS INT),
  CAST(NULL AS DECIMAL(13, 4));
SELECT
  id,
  v,
  IF(v IS NULL, 1, 0) AS is_null_v
FROM sql_tests_write_path.t_decimal_overflow_sink
ORDER BY id;
