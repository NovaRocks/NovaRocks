-- @order_sensitive=true
-- @tags=write_path,dml,temporal
-- Test Objective:
-- 1. Validate DATE/DATETIME values are persisted correctly through INSERT-SELECT into Iceberg sink.
-- 2. Cover leap-day and epoch-style values together with NULL temporal fields.
-- Test Flow:
-- 1. Create/reset temporal source and sink tables.
-- 2. Insert fixed DATE/DATETIME rows (including NULLs) into source table.
-- 3. Insert-select into sink table and assert ordered output.
CREATE DATABASE IF NOT EXISTS sql_tests_write_path;
DROP TABLE IF EXISTS sql_tests_write_path.t_temporal_insert_src;
DROP TABLE IF EXISTS sql_tests_write_path.t_temporal_insert_sink;
CREATE TABLE sql_tests_write_path.t_temporal_insert_src (
  id BIGINT,
  d DATE,
  dt DATETIME
);
CREATE TABLE sql_tests_write_path.t_temporal_insert_sink (
  id INT,
  d DATE,
  dt DATETIME
);
INSERT INTO sql_tests_write_path.t_temporal_insert_src VALUES
  (1, '1970-01-01', '1970-01-01 00:00:00'),
  (2, '2024-02-29', '2024-02-29 23:59:59'),
  (3, NULL, NULL);
INSERT INTO sql_tests_write_path.t_temporal_insert_sink
SELECT id, d, dt
FROM sql_tests_write_path.t_temporal_insert_src;
SELECT id, d, dt
FROM sql_tests_write_path.t_temporal_insert_sink
ORDER BY id;
