-- @order_sensitive=true
-- @tags=project,string
-- Test Objective:
-- 1. Validate string projection functions (CONCAT/COALESCE/UPPER).
-- 2. Prevent regressions in nullable string expression evaluation.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert rows with mixed-case and NULL string fields.
-- 3. Project normalized key expression and assert ordered output.
CREATE DATABASE IF NOT EXISTS sql_tests_d04;
DROP TABLE IF EXISTS sql_tests_d04.t_project_string_functions;
CREATE TABLE sql_tests_d04.t_project_string_functions (
  id INT,
  first_name STRING,
  last_name STRING
);
INSERT INTO sql_tests_d04.t_project_string_functions VALUES
  (1, 'alice', 'smith'),
  (2, 'Bob', 'Lee'),
  (3, NULL, 'Z');
SELECT
  id,
  UPPER(CONCAT(COALESCE(first_name, ''), '_', COALESCE(last_name, ''))) AS full_key
FROM sql_tests_d04.t_project_string_functions
ORDER BY id;
