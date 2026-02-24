-- @order_sensitive=false
-- @tags=write_path,dml,insert_values
-- Test Objective:
-- 1. Validate repeated INSERT ... VALUES writes are visible through SELECT.
-- 2. Prevent regressions where first insert succeeds but subsequent inserts are lost.
-- Test Flow:
-- 1. Create/reset a simple BIGINT table in write-path database.
-- 2. Execute multiple INSERT ... VALUES statements on the same table.
-- 3. Read back all rows and compare as an unordered result set.
CREATE DATABASE IF NOT EXISTS sql_tests_write_path;
DROP TABLE IF EXISTS sql_tests_write_path.t_insert_values_select_repeated;
CREATE TABLE sql_tests_write_path.t_insert_values_select_repeated (
  c1 BIGINT
);
INSERT INTO sql_tests_write_path.t_insert_values_select_repeated VALUES (11);
INSERT INTO sql_tests_write_path.t_insert_values_select_repeated VALUES (22), (33);
INSERT INTO sql_tests_write_path.t_insert_values_select_repeated VALUES (44);
SELECT c1
FROM sql_tests_write_path.t_insert_values_select_repeated;
