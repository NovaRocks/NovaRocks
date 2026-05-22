-- @order_sensitive=true
-- @tags=iceberg_dml,lifecycle
-- Test Objective:
-- 1. Iceberg TRUNCATE TABLE clears rows but keeps the table available for new writes.
-- 2. Iceberg DROP TABLE removes the table from the catalog.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_table_lifecycle;
CREATE TABLE ${case_db}.t_table_lifecycle (
  k1 INT,
  v1 STRING
);
INSERT INTO ${case_db}.t_table_lifecycle VALUES
  (1, 'a'),
  (2, 'b');

-- query 2
SELECT k1, v1
FROM ${case_db}.t_table_lifecycle
ORDER BY k1;

-- query 3
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t_table_lifecycle;

-- query 4
SELECT count(*)
FROM ${case_db}.t_table_lifecycle;

-- query 5
-- @skip_result_check=true
INSERT INTO ${case_db}.t_table_lifecycle VALUES (10, 'z');

-- query 6
SELECT k1, v1
FROM ${case_db}.t_table_lifecycle
ORDER BY k1;

-- query 7
-- @skip_result_check=true
DROP TABLE ${case_db}.t_table_lifecycle;

-- query 8
-- @expect_error=Unknown table
SELECT * FROM ${case_db}.t_table_lifecycle;
