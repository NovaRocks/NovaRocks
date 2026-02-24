-- @order_sensitive=true
-- Test Objective:
-- 1. Validate INSERT VALUES on TINYINT columns with nullable second column.
-- 2. Preserve NULL values and deterministic row ordering on read-back.
-- Test Flow:
-- 1. Create/reset a tinyint table in write-path database.
-- 2. Insert deterministic rows with mixed non-NULL and NULL tinyint values.
-- 3. Query with ORDER BY and compare to expected output.
CREATE DATABASE IF NOT EXISTS sql_tests_write_path;
DROP TABLE IF EXISTS sql_tests_write_path.t_tinyint_insert_values;
CREATE TABLE sql_tests_write_path.t_tinyint_insert_values (
  tinyint_col_1 TINYINT NOT NULL,
  tinyint_col_2 TINYINT
);
INSERT INTO sql_tests_write_path.t_tinyint_insert_values VALUES
  (1, 1),
  (1, 2),
  (1, 3),
  (1, 4),
  (2, NULL),
  (3, NULL),
  (4, NULL);
SELECT tinyint_col_1, tinyint_col_2
FROM sql_tests_write_path.t_tinyint_insert_values
ORDER BY tinyint_col_1, tinyint_col_2 IS NULL, tinyint_col_2;
