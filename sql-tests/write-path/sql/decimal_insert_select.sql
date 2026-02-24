-- @order_sensitive=true
-- @tags=write_path,dml,decimal
-- Test Objective:
-- 1. Validate INSERT-SELECT writing DECIMAL values from a wider DECIMAL source into a narrower sink schema.
-- 2. Validate NULL propagation for DECIMAL columns through Iceberg table sink.
-- Test Flow:
-- 1. Create/reset DECIMAL source and sink tables in write-path database.
-- 2. Insert deterministic rows (positive/negative/NULL) into source table.
-- 3. Insert into sink by SELECT from source and verify ordered read-back.
CREATE DATABASE IF NOT EXISTS sql_tests_write_path;
DROP TABLE IF EXISTS sql_tests_write_path.t_decimal_insert_src;
DROP TABLE IF EXISTS sql_tests_write_path.t_decimal_insert_sink;
CREATE TABLE sql_tests_write_path.t_decimal_insert_src (
  id BIGINT,
  v DECIMAL(20, 6)
);
CREATE TABLE sql_tests_write_path.t_decimal_insert_sink (
  id INT,
  v DECIMAL(10, 3)
);
INSERT INTO sql_tests_write_path.t_decimal_insert_src VALUES
  (1, 123.456000),
  (2, -99.125000),
  (3, NULL);
INSERT INTO sql_tests_write_path.t_decimal_insert_sink
SELECT id, v
FROM sql_tests_write_path.t_decimal_insert_src;
SELECT id, v
FROM sql_tests_write_path.t_decimal_insert_sink
ORDER BY id;
