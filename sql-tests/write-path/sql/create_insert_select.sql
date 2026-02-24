-- @order_sensitive=true
-- Test Objective:
-- 1. Validate CREATE TABLE and multi-row INSERT for primitive + nullable values.
-- 2. Validate deterministic read-back for inserted rows.
-- Test Flow:
-- 1. Create/reset a basic table in the write-path database.
-- 2. Insert fixed rows including a NULL value.
-- 3. Read rows with ORDER BY and compare with expected result.
CREATE DATABASE IF NOT EXISTS sql_tests_write_path;
DROP TABLE IF EXISTS sql_tests_write_path.t_basic;
CREATE TABLE sql_tests_write_path.t_basic (
  id BIGINT,
  name STRING,
  qty BIGINT
);
INSERT INTO sql_tests_write_path.t_basic VALUES
  (1, 'apple', 10),
  (2, 'banana', 20),
  (3, 'banana', NULL);
SELECT id, name, qty
FROM sql_tests_write_path.t_basic
ORDER BY id;
