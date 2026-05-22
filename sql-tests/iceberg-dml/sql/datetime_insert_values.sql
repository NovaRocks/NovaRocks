-- @order_sensitive=true
-- @tags=iceberg_dml,datetime
-- Test Objective:
-- 1. Validate DATETIME writes through direct VALUES and INSERT-SELECT constant expressions.
-- 2. Validate derived function result (YEAR) is consistent after sink persistence.
DROP TABLE IF EXISTS ${case_db}.t_datetime_insert_values;
CREATE TABLE ${case_db}.t_datetime_insert_values (
  id INT,
  dt DATETIME
);
INSERT INTO ${case_db}.t_datetime_insert_values VALUES
  (1, '2024-03-01 10:20:30');
INSERT INTO ${case_db}.t_datetime_insert_values
SELECT 2, CAST('2024-12-31 23:59:59' AS DATETIME);
INSERT INTO ${case_db}.t_datetime_insert_values
SELECT 3, NULL;
SELECT id, dt, YEAR(dt) AS y
FROM ${case_db}.t_datetime_insert_values
ORDER BY id;
