-- @order_sensitive=true
-- @tags=write_path,dml,decimal,rounding
-- Test Objective:
-- 1. Validate decimal scale narrowing writes apply deterministic rounding for positive and negative values.
-- 2. Validate NULL propagation for decimal rows during sink writes.
-- Test Flow:
-- 1. Create/reset decimal sink table with target scale.
-- 2. Insert deterministic rows via typed SELECT constants using a wider decimal scale.
-- 3. Query ordered output and assert rounded sink values.
SET enable_scan_datacache = false;
SET enable_datacache_io_adaptor = false;
SET enable_populate_datacache = false;
SET enable_datacache_async_populate_mode = false;
SET enable_spill = false;
CREATE DATABASE IF NOT EXISTS sql_tests_write_path;
DROP TABLE IF EXISTS sql_tests_write_path.t_decimal_rounding_sink;
CREATE TABLE sql_tests_write_path.t_decimal_rounding_sink (
  id INT,
  v DECIMAL(10, 2)
);
INSERT INTO sql_tests_write_path.t_decimal_rounding_sink
SELECT CAST(1 AS INT), CAST(1.2344 AS DECIMAL(10, 4));
INSERT INTO sql_tests_write_path.t_decimal_rounding_sink
SELECT CAST(2 AS INT), CAST(1.2356 AS DECIMAL(10, 4));
INSERT INTO sql_tests_write_path.t_decimal_rounding_sink
SELECT CAST(3 AS INT), CAST(-2.3444 AS DECIMAL(10, 4));
INSERT INTO sql_tests_write_path.t_decimal_rounding_sink
SELECT CAST(4 AS INT), CAST(-2.3456 AS DECIMAL(10, 4));
INSERT INTO sql_tests_write_path.t_decimal_rounding_sink
SELECT CAST(5 AS INT), CAST(NULL AS DECIMAL(10, 4));
SELECT id, v
FROM sql_tests_write_path.t_decimal_rounding_sink
ORDER BY id;
