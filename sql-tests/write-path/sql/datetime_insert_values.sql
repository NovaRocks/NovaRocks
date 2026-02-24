-- @order_sensitive=true
-- @tags=write_path,dml,datetime
-- Test Objective:
-- 1. Validate DATETIME writes through direct VALUES and INSERT-SELECT constant expressions.
-- 2. Validate derived function result (YEAR) is consistent after sink persistence.
-- Test Flow:
-- 1. Create/reset DATETIME sink table in write-path database.
-- 2. Insert deterministic DATETIME values through different insertion forms.
-- 3. Query DATETIME and YEAR(dt) to assert persisted values and null behavior.
-- Note:
-- 1. TIMESTAMP type syntax is not accepted in current SQL frontend; DATETIME is used as timestamp-semantics coverage.
CREATE DATABASE IF NOT EXISTS sql_tests_write_path;
DROP TABLE IF EXISTS sql_tests_write_path.t_datetime_insert_values;
CREATE TABLE sql_tests_write_path.t_datetime_insert_values (
  id INT,
  dt DATETIME
);
INSERT INTO sql_tests_write_path.t_datetime_insert_values VALUES
  (1, '2024-03-01 10:20:30');
INSERT INTO sql_tests_write_path.t_datetime_insert_values
SELECT 2, CAST('2024-12-31 23:59:59' AS DATETIME);
INSERT INTO sql_tests_write_path.t_datetime_insert_values
SELECT 3, NULL;
SELECT id, dt, YEAR(dt) AS y
FROM sql_tests_write_path.t_datetime_insert_values
ORDER BY id;
