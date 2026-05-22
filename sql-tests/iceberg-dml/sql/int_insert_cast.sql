-- @order_sensitive=true
-- @tags=iceberg_dml,cast
-- Test Objective:
-- 1. Regression coverage for writing INT target columns through Iceberg sink.
-- 2. Validate INT INSERT path through a MySQL-style user variable.
DROP TABLE IF EXISTS ${case_db}.t_int_insert_regression;
CREATE TABLE ${case_db}.t_int_insert_regression (
  id INT,
  v INT
);
SET @i = 1;
INSERT INTO ${case_db}.t_int_insert_regression VALUES (@i, @i);
INSERT INTO ${case_db}.t_int_insert_regression VALUES (2, 2);
INSERT INTO ${case_db}.t_int_insert_regression VALUES (3, 3);
SELECT id, v
FROM ${case_db}.t_int_insert_regression
ORDER BY id;
