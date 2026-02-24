-- @order_sensitive=true
-- Test Objective:
-- 1. Regression coverage for writing INT target columns through Iceberg sink.
-- 2. Validate implicit type alignment from INT64-producing expressions to INT schema.
-- Test Flow:
-- 1. Create/reset INT target table and BIGINT source table.
-- 2. Insert into INT table using a session variable and numeric literals.
-- 3. Insert into INT table via INSERT-SELECT from BIGINT source table.
-- 4. Query ordered rows and verify all expected values are persisted.
CREATE DATABASE IF NOT EXISTS sql_tests_write_path;
DROP TABLE IF EXISTS sql_tests_write_path.t_int_insert_regression;
DROP TABLE IF EXISTS sql_tests_write_path.t_int_insert_src;
CREATE TABLE sql_tests_write_path.t_int_insert_regression (
  id INT,
  v INT
);
CREATE TABLE sql_tests_write_path.t_int_insert_src (
  id BIGINT,
  v BIGINT
);
SET @i = 1;
INSERT INTO sql_tests_write_path.t_int_insert_regression VALUES (@i, @i);
INSERT INTO sql_tests_write_path.t_int_insert_regression VALUES (2, 2);
INSERT INTO sql_tests_write_path.t_int_insert_src VALUES (3, 3);
INSERT INTO sql_tests_write_path.t_int_insert_regression
SELECT id, v
FROM sql_tests_write_path.t_int_insert_src;
SELECT id, v
FROM sql_tests_write_path.t_int_insert_regression
ORDER BY id;
