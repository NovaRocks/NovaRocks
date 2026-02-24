-- @order_sensitive=true
-- Test Objective:
-- 1. Validate basic DDL execution in the hadoop catalog.
-- 2. Validate metadata visibility immediately after database creation.
-- Test Flow:
-- 1. Reset a dedicated metadata database for this case.
-- 2. Query metadata with SHOW DATABASES and assert the created name appears.
DROP DATABASE IF EXISTS sql_tests_write_path_meta;
CREATE DATABASE sql_tests_write_path_meta;
SHOW DATABASES LIKE 'sql_tests_write_path_meta';
