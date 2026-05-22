-- @order_sensitive=true
-- @tags=iceberg_dml,delete
-- Test Objective:
-- 1. Validate Iceberg v3 table accepts INSERT then DELETE and exposes correct visible rows.
-- 2. Prevent regression where DELETE is accepted but reads still return the deleted row.
DROP TABLE IF EXISTS ${case_db}.t_insert_delete_select;
CREATE TABLE ${case_db}.t_insert_delete_select (
  city_id INT,
  population INT,
  city STRING
) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.t_insert_delete_select VALUES
  (1, 100, 'Beijing'),
  (2, 200, 'Shanghai'),
  (3, 300, 'Shenzhen');
DELETE FROM ${case_db}.t_insert_delete_select
WHERE city_id = 2;
SELECT city_id, population, city
FROM ${case_db}.t_insert_delete_select
ORDER BY city_id;
